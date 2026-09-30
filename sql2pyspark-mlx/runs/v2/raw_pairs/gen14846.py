from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "category",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "supplier_id", "launch_date", "category")
)
