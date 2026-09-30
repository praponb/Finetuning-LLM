from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "customer_id",
        "country",
        "started_at",
        "browser",
        F.first("browser").over(w).alias("first_browser"),
    )
    .orderBy("customer_id")
)
