from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("age").desc_nulls_last())
w2 = Window.orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("age", "id")

enriched = (
    df_sessions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.signup_date") >= "2023-12-01")
    .select(
        "e.id",
        "e.customer_id",
        "e.country",
        F.col("c.email"),
        F.col("c.age"),
        F.col("c.city"),
    )
)

filtered = (
    enriched
    .filter(F.col("email").isNull())
    .select(
        "id",
        "customer_id",
        "email",
        "age",
        "city",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

valid = (
    filtered
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "customer_id",
        "email",
        "age",
        "city",
        "email_domain",
        F.col("age").cast("string").alias("age_num"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "customer_id",
        "age",
        "city",
        "email_domain",
        "age_num",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.avg("age").over(w2).alias("age_avg_5"),
        F.ntile(10).over(w3).alias("age_bucket"),
    )
)

result = (
    metrics
    .groupBy(F.when(F.col("age") >= 62, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("age_avg_5")).otherwise(0)).alias("new_york_age_avg_5"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.round(F.avg("age_avg_5"), 2).alias("avg_age_avg_5"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("avg_age_avg_5").desc_nulls_first())
)
