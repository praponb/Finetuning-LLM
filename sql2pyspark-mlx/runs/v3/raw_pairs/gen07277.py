from pyspark.sql import functions as F

src = df_subscriptions.filter((F.col("customer_id") == 36) & (F.col("plan") != "premium"))

summary = (
    src
    .groupBy("plan", F.year("start_date").alias("start_year"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = summary.orderBy(F.col("plan").desc())
