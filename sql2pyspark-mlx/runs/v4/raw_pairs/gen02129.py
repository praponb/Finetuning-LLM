from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard"]) & ((F.col("plan") != "basic") | F.col("start_date").between("2025-01-16", "2025-02-05")))
    .orderBy(F.col("start_date").desc(), "id")
    .limit(50)
)
