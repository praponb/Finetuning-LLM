from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "browser",
        F.last("browser").over(w).alias("last_browser"),
    )
)
