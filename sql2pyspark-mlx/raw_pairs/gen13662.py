from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("order_discount").desc(), "id")
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("order_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.amount") < 1050), "left_semi")
)

filtered = (
    eligible
    .filter(F.col("city") == "London")
    .withColumn("name_length", F.length("name"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("UNKNOWN")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.credit_limit",
        "s.resolved_segment",
        F.col("c.discount").alias("order_discount"),
    )
)

enriched = (
    latest_match
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "name",
        "segment",
        "credit_limit",
        "order_discount",
        F.ntile(3).over(w2).alias("order_discount_bucket"),
        F.avg("order_discount").over(w3).alias("order_discount_avg_6"),
        F.sum("order_discount").over(w4).alias("running_order_discount"),
        F.avg("order_discount").over(w5).alias("country_avg_order_discount"),
        F.avg("order_discount").over(w3).alias("order_discount_avg_6_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "name",
        "credit_limit",
        "order_discount_avg_6",
        "running_order_discount",
        "country_avg_order_discount",
        "order_discount_avg_6_2",
        F.when(F.col("name").rlike("^(Alice Wong|FRANK MILLER)$"), "GROUP_A").when(F.col("name").rlike("^(bob smith|  Carol Diaz)$"), "EXTENDED").otherwise("CORE").alias("name_zone"),
    )
    .orderBy(F.col("country_avg_order_discount").asc_nulls_first(), "name_zone")
)
