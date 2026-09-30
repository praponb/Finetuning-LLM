from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-07-01", "2025-09-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.lead("units", 1).over(w).alias("next_units"),
    )
    .orderBy(F.col("units").desc())
)
