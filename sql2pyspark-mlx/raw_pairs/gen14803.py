from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("created_at", "id", "k")
w4 = Window.partitionBy("k").orderBy("row_seq", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("row_seq", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.col("customer_id").isNotNull())
    .withColumn("k_filled", F.coalesce("k", F.lit("unassigned")))
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "k",
        "attr_value",
        "k_filled",
        F.row_number().over(w).alias("row_seq"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.row_number().over(w).alias("row_seq_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

enriched_carts = (
    enriched
    .select(
        "id",
        "customer_id",
        "k",
        "attr_value",
        "row_seq",
        F.max("event_seq").over(w4).alias("running_max_event_seq"),
        F.min("event_seq").over(w4).alias("running_min_event_seq"),
        F.count("event_seq").over(w4).alias("running_count_event_seq"),
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
    )
)

result = (
    enriched_carts
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "k",
        "attr_value",
        "running_min_event_seq",
        "running_count_event_seq",
        "last_known_event_seq",
        F.when(F.col("k").rlike("^(utm_source|device)$"), "EXTENDED").otherwise("PRIORITY").alias("k_zone"),
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("running_min_event_seq").desc_nulls_first())
)
