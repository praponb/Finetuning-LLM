from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Chrome", "Firefox"]))
    .select(
        "id",
        "browser",
        "started_at",
        "customer_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "browser", "started_at", "customer_id")
)
