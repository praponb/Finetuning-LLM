from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "supplier_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "category", "launch_date", "supplier_id")
    .orderBy(F.col("supplier_id").desc())
)
