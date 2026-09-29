from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("signup_date"), "segment").orderBy("signup_date", "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("signup_date"), "city").orderBy("age", "id")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("country_lower", F.lower("country"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.year(F.col("ch.opened_date")) >= 2023), "left_anti")
)

scored = (
    eligible
    .filter(F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "country_lower",
        F.first("credit_limit", ignorenulls=True).over(w_recent).alias("first_known_credit_limit"),
        F.lead("credit_limit", 1).over(w2).alias("next_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
        F.lag("credit_limit", 1, 0).over(w4).alias("prev_credit_limit"),
    )
)

kept_keys = (
    scored
    .select("segment", "id")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("segment", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible_customers = (
    kept_rows
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.monthly_fee") > 80), "left_anti")
)

matched_keys = (
    eligible_customers
    .select("segment")
    .intersect(
        df_customers
        .filter(F.length("email") >= 5)
        .select("segment")
    )
)

kept_rows_customers = (
    eligible_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows_customers
    .filter((F.col("next_credit_limit") < 9500) & (F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "segment",
        "country_lower",
        "first_known_credit_limit",
        "next_credit_limit",
        "last_known_credit_limit",
        "prev_credit_limit",
        F.substring("city", 1, 3).alias("city_prefix"),
    )
)

summary = (
    filtered
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("last_known_credit_limit")).otherwise(0)).alias("consumer_last_known_credit_limit"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
    )
)

result = (
    summary
    .select(
        "segment",
        "consumer_last_known_credit_limit",
        "smb_count",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "GROUP_B").when(F.col("segment").rlike("^(Consumer)$"), "OTHER_ZONE").otherwise("CORE").alias("segment_zone"),
    )
)
