from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("customer_id").isin([26, 35]))
    .select(
        "id",
        "country",
        "started_at",
        F.row_number().over(w).alias("started_at_row"),
    )
)
