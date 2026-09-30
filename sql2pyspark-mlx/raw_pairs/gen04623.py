from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w_ordered = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([13, 35])), "left_anti")
)

base_customers = (
    kept
    .filter(F.col("city").isNotNull() & (F.col("country") == "JP"))
    .select(
        "id",
        "country",
        "segment",
        "credit_limit",
        "resolved_city",
        "dedup_rank",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
    )
)

result = (
    base_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        "dedup_rank",
        "email_slug",
        F.count("credit_limit").over(w_ordered).alias("running_count_credit_limit"),
        F.sum("credit_limit").over(w_ordered).alias("running_sum_credit_limit"),
        F.avg("credit_limit").over(w_ordered).alias("running_avg_credit_limit"),
        F.max("credit_limit").over(w_ordered).alias("running_max_credit_limit"),
    )
)
