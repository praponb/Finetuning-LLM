from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_dayofweek_tier = Window.partitionBy("hired_on_dayofweek", "tier")
w_tier_len_by_tier_len = Window.partitionBy("tier_len").orderBy(F.col("tier_len").asc_nulls_last(), "id")
w_tier_len_by_tier_len_running = Window.partitionBy("tier_len").orderBy(F.col("tier_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 6)
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.dayofweek(F.col("hired_on")).alias("hired_on_dayofweek"),
        F.length(F.trim(F.col("tier"))).alias("tier_len"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.round(F.avg("hired_on_dayofweek").over(w_hired_on_dayofweek_tier), 2).alias("hired_on_dayofweek_tier_avg_hired_on_dayofweek"),
        F.sum("tier_len").over(w_hired_on_dayofweek_tier).alias("hired_on_dayofweek_tier_sum_tier_len"),
        F.lag("hired_on_dayofweek", 2, 0).over(w_tier_len_by_tier_len).alias("prev2_hired_on_dayofweek"),
        F.last("tier_len", ignorenulls=True).over(w_tier_len_by_tier_len_running).alias("last_known_tier_len"),
    )
)

tagged = (
    ranked
    .filter(F.col("tier_len").between(4, 12))
    .withColumn("tier_len_2", F.length(F.trim(F.col("tier"))))
)

result = (
    tagged
    .cube("prev2_hired_on_dayofweek", "tier_len_2")
    .agg(
        F.max("hired_on").alias("last_hired_on"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("prev2_hired_on_dayofweek").asc_nulls_last(),
        F.col("tier_len_2").asc_nulls_last(),
    )
)
