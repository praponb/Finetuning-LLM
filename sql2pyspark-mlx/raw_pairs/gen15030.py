from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("city")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

enriched = (
    lines
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.coupon_codes",
        "a.attr_value",
        F.col("d.credit_limit"),
    )
)

base = (
    enriched
    .filter(F.hour("created_at").between(8, 13))
    .select(
        "id",
        "customer_id",
        "attr_value",
        "credit_limit",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
    )
)

enriched_carts = (
    base
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.name"), F.col("p.city"))
)

scored = (
    enriched_carts
    .select(
        "customer_id",
        "attr_value",
        "name",
        "city",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.stddev_samp("credit_limit").over(w2).alias("city_stddev_samp_credit_limit"),
    )
)

result = (
    scored
    .select(
        "attr_value",
        "city",
        "credit_limit_share",
        "city_stddev_samp_credit_limit",
        F.when(F.col("city").rlike("^(Singapore|Tokyo)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("city_stddev_samp_credit_limit")
)
