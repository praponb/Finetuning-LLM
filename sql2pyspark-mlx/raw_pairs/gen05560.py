from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("age").desc_nulls_last(), "id")
w2 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit_scaled").desc_nulls_last(), "id")
w3 = Window.partitionBy("segment").orderBy("credit_limit_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("signup_date"), "country").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

deduped = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_semi")
)

valid = (
    not_flagged
    .filter((F.col("signup_date") > F.lit("2023-06-23").cast("date")) & (~F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "*",
        F.round((F.col("credit_limit") * 0.85), 2).alias("credit_limit_scaled"),
        F.weekofyear("signup_date").alias("signup_date_weekofyear"),
    )
)

deduped_customers = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < "2023-10-30"), "left_anti")
)

result = (
    kept
    .filter(F.col("is_active"))
    .select(
        "*",
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(10).over(w5).alias("credit_limit_bucket"),
    )
)
