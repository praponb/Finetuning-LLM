from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "country",
        "started_at",
        F.ntile(10).over(w).alias("started_at_ntile"),
    )
)
