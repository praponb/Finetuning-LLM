from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country") != "JP")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.row_number().over(w).alias("started_at_row"),
    )
    .orderBy("started_at_row")
)
