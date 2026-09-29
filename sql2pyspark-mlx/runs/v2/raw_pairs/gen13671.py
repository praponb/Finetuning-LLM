from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
    )
    .orderBy(F.col("id").desc())
)
