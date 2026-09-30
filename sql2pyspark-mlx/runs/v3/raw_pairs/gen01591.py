from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select(
        "id",
        "customer_id",
        "started_at",
        F.ntile(3).over(w).alias("started_at_ntile"),
    )
)
