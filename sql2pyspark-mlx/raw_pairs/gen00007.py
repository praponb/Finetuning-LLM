from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "customer_id",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
    )
    .orderBy("customer_id", "next_started_at")
)
