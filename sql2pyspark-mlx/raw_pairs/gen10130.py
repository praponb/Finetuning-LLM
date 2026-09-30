from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment")

not_flagged = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.month(F.col("c.end_date")).isin([5, 7, 11])), "left_semi")
)

scored = (
    not_flagged
    .filter(F.col("city") != "London")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.count("credit_limit").over(w2).alias("segment_count_credit_limit"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_semi")
)

kept_keys = (
    kept
    .select("segment", "id")
    .exceptAll(
        df_customers
        .filter(F.col("is_active"))
        .select("segment", "id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age_share",
    )
    .orderBy("country")
)
