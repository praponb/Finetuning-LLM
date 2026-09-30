from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 6]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.lead("units", 2).over(w).alias("next_units"),
    )
    .orderBy(F.col("id").desc())
)
