from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "browser").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select(
        "id",
        "started_at",
        "browser",
        "customer_id",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.percent_rank().over(w2).alias("started_at_pct_rank"),
    )
    .orderBy("browser", F.col("dense_rnk").desc())
)
