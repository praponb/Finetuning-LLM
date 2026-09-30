from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.lead("units", 1).over(w).alias("next_units"),
    )
)
