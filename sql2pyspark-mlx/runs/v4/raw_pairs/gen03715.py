from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id", "promo_id").rowsBetween(-3, -1)
w2 = Window.orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("order_date", "id", "promo_id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .withColumn("status_length", F.length("status"))
)

matched = (
    cleaned
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    matched
    .withColumn("shipping_fee_avg_3_prior", F.avg("shipping_fee").over(w))
    .withColumn("cumulative_status_length", F.sum("status_length").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

cleaned_orders = (
    scored
    .filter((F.col("order_date") > F.lit("2023-07-20").cast("date")) & F.col("promo_code").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "status_length",
        "promo_id",
        "promo_code",
        "discount_pct",
        "shipping_fee_avg_3_prior",
        "seq_no",
        F.round((F.col("promo_id") / F.col("seq_no")), 2).alias("promo_id_per_seq_no"),
        F.coalesce("promo_code", F.lit("N/A")).alias("promo_code_clean"),
    )
)

result = (
    cleaned_orders
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "discount",
        "status_length",
        "promo_id",
        "promo_code",
        "shipping_fee_avg_3_prior",
        "seq_no",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("seq_no").asc_nulls_last())
)
