from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency")
w_part_2 = Window.orderBy("element")
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w4 = Window.orderBy(F.col("element_cume_dist").asc_nulls_last())
w5 = Window.orderBy("element_cume_dist")
w6 = Window.orderBy(F.col("currency_sum_currency_sum_element").desc_nulls_first())
w7 = Window.partitionBy("plan")
w8 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_last())
w9 = Window.partitionBy("currency").orderBy(F.col("currency_sum_currency_sum_element").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

enriched = (
    lines
    .select(
        "*",
        F.sum("element").over(w_part).alias("currency_sum_element"),
        F.cume_dist().over(w_part_2).alias("element_cume_dist"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)

metrics = (
    enriched
    .filter(F.col("element_dense_rank").between(3, 7))
    .withColumn("currency_sum_currency_sum_element", F.sum("currency_sum_element").over(w_part))
    .withColumn("element_cume_dist_percent_rank", F.percent_rank().over(w4))
    .withColumn("element_cume_dist_percent_rank_2", F.percent_rank().over(w5))
)

scored = (
    metrics
    .withColumn("currency_sum_currency_sum_element_dense_rank", F.dense_rank().over(w6))
    .withColumn("plan_avg_element", F.avg("element").over(w7))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w8))
    .withColumn("currency_sum_currency_sum_element_dense_rank_2", F.dense_rank().over(w9))
    .withColumn("currency_count_element_cume_dist_percent_rank_2", F.count("element_cume_dist_percent_rank_2").over(w_part))
)

filtered = (
    scored
    .filter(F.col("currency").isin(["USD", "EUR", "THB"]))
    .withColumn("greatest_element_cume_dist_currency_sum_element", F.greatest("element_cume_dist", "currency_sum_element"))
)

kept_keys = (
    filtered
    .select("currency", "plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("currency", "plan")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "plan",
        "greatest_element_cume_dist_currency_sum_element",
        "element_cume_dist",
        F.col("element_cume_dist_percent_rank_2").alias("ELEMENT_CUME_DIST_PERCENT_RANK_2_VAL"),
        F.col("element_cume_dist_percent_rank").alias("ELEMENT_CUME_DIST_PERCENT_RANK_VAL"),
    )
    .unpivot(
        ids=["plan"],
        values=["greatest_element_cume_dist_currency_sum_element", "element_cume_dist", "ELEMENT_CUME_DIST_PERCENT_RANK_2_VAL", "ELEMENT_CUME_DIST_PERCENT_RANK_VAL"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
