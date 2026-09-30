from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount_per_amount", "id")
w2 = Window.orderBy("amount", "id")
w3 = Window.orderBy("least_amount_shipping_fee", "id")
w4 = Window.orderBy("prev_least_amount_shipping_fee", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-01").cast("date")) & F.col("amount").between(1300, 1440))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.least("amount", "shipping_fee").alias("least_amount_shipping_fee"),
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "amount",
        "discount",
        "least_amount_shipping_fee",
        "discount_per_amount",
        F.lag("least_amount_shipping_fee").over(w).alias("prev_least_amount_shipping_fee"),
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.lag("discount_per_amount", 1).over(w2).alias("prev_discount_per_amount"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "discount_per_amount",
        "next_amount",
        F.lag("prev_discount_per_amount").over(w3).alias("prev_prev_discount_per_amount"),
        F.lead("least_amount_shipping_fee").over(w3).alias("next_least_amount_shipping_fee"),
        F.lag("least_amount_shipping_fee").over(w3).alias("prev_least_amount_shipping_fee_2"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("discount_per_amount").over(w4).alias("discount_per_amount_avg_3"),
    )
)

valid = (
    enriched
    .filter(F.col("next_amount") <= 1580)
    .select(
        "id",
        "discount_per_amount",
        "next_amount",
        "prev_least_amount_shipping_fee_2",
        "row_seq",
        "discount_per_amount_avg_3",
        F.col("discount_per_amount_avg_3").cast("string").alias("discount_per_amount_avg_3_str"),
        (F.col("next_amount") / F.nullif("discount_per_amount", F.lit(0))).alias("next_amount_per_discount_per_amount"),
    )
)

base = (
    valid
    .filter(F.col("next_amount_per_discount_per_amount").isNotNull())
    .select(
        "id",
        "discount_per_amount",
        "prev_least_amount_shipping_fee_2",
        "row_seq",
        "discount_per_amount_avg_3",
        "discount_per_amount_avg_3_str",
        "next_amount_per_discount_per_amount",
        (F.col("discount_per_amount") * F.col("row_seq")).alias("discount_per_amount_times_row_seq"),
        F.least("row_seq", "discount_per_amount_avg_3").alias("least_row_seq_discount_per_amount_avg_3"),
    )
)

result = (
    base
    .select(
        "id",
        "discount_per_amount_times_row_seq",
        F.col("prev_least_amount_shipping_fee_2").alias("PREV_LEAST_AMOUNT_SHIPPING_FEE_2_VAL"),
        F.col("next_amount_per_discount_per_amount").alias("NEXT_AMOUNT_PER_DISCOUNT_PER_AMOUNT"),
    )
    .unpivot(
        ids=["id"],
        values=["discount_per_amount_times_row_seq", "PREV_LEAST_AMOUNT_SHIPPING_FEE_2_VAL", "NEXT_AMOUNT_PER_DISCOUNT_PER_AMOUNT"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
