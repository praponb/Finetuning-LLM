from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select(
        "id",
        "country",
        "started_at",
        "customer_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
)
