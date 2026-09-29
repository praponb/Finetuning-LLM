from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")
w2 = Window.partitionBy("segment")
w3 = Window.orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("city", "segment")

eligible = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.year(F.col("x.end_date")) >= 2024), "left_anti")
)

matched_keys = (
    eligible
    .select("segment", "id")
    .exceptAll(
        df_customers
        .filter(F.col("age").isNotNull())
        .select("segment", "id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") < F.col("s.signup_date")), "left_semi")
)

scored = (
    not_flagged
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w2))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w3))
)

scored_customers = (
    scored
    .filter(F.col("corr_credit_limit_age") >= -0.0)
    .withColumn("city_avg_corr_age_credit_limit", F.avg("corr_age_credit_limit").over(w4))
)

result = (
    scored_customers
    .select("country", "corr_age_credit_limit", "credit_limit_cume_dist")
    .unpivot(
        ids=["country"],
        values=["corr_age_credit_limit", "credit_limit_cume_dist"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
