from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .select(
        "id",
        "country",
        "customer_id",
        "started_at",
        "browser",
        "device",
        F.ntile(3).over(w).alias("bucket"),
        F.last("device").over(w2).alias("last_device"),
    )
)
