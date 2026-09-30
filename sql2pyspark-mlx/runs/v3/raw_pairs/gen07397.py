from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "revenue",
        F.avg("revenue").over(w).alias("store_id_avg_revenue"),
    )
    .orderBy("store_id", F.col("revenue").desc())
)
