from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc())

metrics = (
    df_accounts
    .select(
        "*",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.percentile_approx("balance_cume_dist", [0.25, 0.5, 0.75]).alias("balance_cume_dist_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("year_start").desc())
)
