from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "brand",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "supplier_id", "launch_date", "brand")
    .orderBy(F.col("supplier_id").desc())
)
