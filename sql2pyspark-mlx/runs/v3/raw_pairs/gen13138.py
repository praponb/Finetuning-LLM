from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "browser",
        "started_at",
        "device",
        F.first("device").over(w).alias("first_device"),
    )
)
