from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") != "Safari")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("rn_2"),
    )
    .orderBy("country")
)
