from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "category",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "supplier_id", "launch_date", "category")
    .orderBy(F.col("launch_date").desc())
)
