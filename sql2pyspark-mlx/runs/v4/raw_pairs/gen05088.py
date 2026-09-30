from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy(F.col("days_since_created_at").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("days_since_created_at").desc())
w_running = Window.partitionBy("customer_id").orderBy(F.col("days_since_created_at").asc_nulls_last())
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("days_since_created_at_cume_dist").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

base = (
    exploded
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

base_carts = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "days_since_created_at",
        "key_clean",
        F.coalesce("days_since_created_at", F.lit(0)).alias("days_since_created_at_filled"),
        F.when(F.col("days_since_created_at") >= 82, "High").when(F.col("days_since_created_at") >= 14, "Medium").otherwise("Low").alias("days_since_created_at_band"),
    )
)

scored = (
    base_carts
    .withColumn("days_since_created_at_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("days_since_created_at_rank", F.rank().over(w2))
    .withColumn("days_since_created_at_cume_dist", F.cume_dist().over(w_running))
)

scored_carts = (
    scored
    .withColumn("days_since_created_at_rank_share", F.round((F.col("days_since_created_at_rank") / F.sum("days_since_created_at_rank").over(w4)), 4))
    .withColumn("days_since_created_at_cume_dist_dense_rank", F.dense_rank().over(w5))
    .withColumn("days_since_created_at_dense_rank_2", F.dense_rank().over(w_running))
)

result = (
    scored_carts
    .select(
        "customer_id",
        "days_since_created_at",
        "key_clean",
        "days_since_created_at_filled",
        "days_since_created_at_band",
        "days_since_created_at_cume_dist",
        "days_since_created_at_rank_share",
        "days_since_created_at_cume_dist_dense_rank",
        "days_since_created_at_dense_rank_2",
    )
    .orderBy(F.col("days_since_created_at_cume_dist").desc())
)
