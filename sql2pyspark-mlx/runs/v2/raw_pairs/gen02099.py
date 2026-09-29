from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date") > "2024-05-01")
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "product_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "supplier_id", "launch_date", "product_name")
    .orderBy("supplier_id")
)
