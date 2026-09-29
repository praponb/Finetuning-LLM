from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
