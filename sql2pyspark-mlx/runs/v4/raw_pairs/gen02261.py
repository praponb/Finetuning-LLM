from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w2 = Window.partitionBy("country", "city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy("age", "id")
w5 = Window.orderBy(F.col("age").desc_nulls_last())
w_ordered = Window.partitionBy("country").orderBy("credit_limit", "id")
w_part = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit"),
        F.sum("age").over(w3).alias("running_age"),
    )
)

scored = (
    enriched
    .withColumn("next_running_age", F.lead("running_age", 1).over(w4))
    .withColumn("age_dense_rank", F.dense_rank().over(w5))
    .withColumn("credit_limit_bucket", F.ntile(10).over(w_ordered))
    .withColumn("running_age_2", F.sum("age").over(w_part))
)

eligible = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") <= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .groupBy(F.when(F.col("age") >= 68, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("jp_events") >= 1)
    .orderBy(F.col("row_count").desc())
)
