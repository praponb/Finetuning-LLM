from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "country", "started_at", "browser")
    .orderBy("country")
)
