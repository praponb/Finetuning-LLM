from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code", "id", "k")
w2 = Window.partitionBy("service").orderBy("status_code", "id", "k")
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "service", "status_code", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .select(
        "id",
        "status_code",
        "k",
        "v",
        F.lead("status_code", 1, 0).over(w).alias("next_status_code"),
        F.lag("status_code", 1, 0).over(w2).alias("prev_status_code"),
        F.last("k", ignorenulls=True).over(w3).alias("last_known_k"),
        F.ntile(3).over(w2).alias("status_code_bucket"),
        F.ntile(5).over(w2).alias("status_code_bucket_2"),
    )
)
