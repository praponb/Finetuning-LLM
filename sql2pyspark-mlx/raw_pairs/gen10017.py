from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy(F.month("signup_date"), "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w4 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") < "2025-06-01")
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lead("age", 2).over(w).alias("next_age"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
        F.lead("credit_limit", 1).over(w3).alias("next_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w4).alias("last_known_credit_limit"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "city",
        "last_known_credit_limit",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
    )
    .unpivot(
        ids=["id", "city"],
        values=["last_known_credit_limit", "CREDIT_LIMIT_VAL"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
