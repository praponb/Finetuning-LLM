from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("quantity_whole_rounded", "id")
w2 = Window.orderBy("quantity_whole_whole", "id")
w3 = Window.orderBy("seq_no", "id").rowsBetween(-2, -1)
w4 = Window.orderBy(F.col("seq_no").asc_nulls_first())
w5 = Window.orderBy(F.col("seq_no").desc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

base = (
    valid
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "last_restock",
        "quantity_whole",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

cleaned = (
    base
    .filter(F.col("last_restock") >= F.lit("2023-02-09").cast("date"))
    .select(
        "id",
        "quantity_whole",
        "last_restock_label",
        F.round("quantity_whole", 1).alias("quantity_whole_rounded"),
        F.ceil("quantity_whole").alias("quantity_whole_whole"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("quantity_whole_rounded").over(w_grp).alias("prev_quantity_whole_rounded"),
        F.lead("quantity_whole").over(w_grp).alias("next_quantity_whole"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

filtered = (
    metrics
    .filter(F.col("quantity_whole") != 21)
    .select(
        "id",
        "quantity_whole",
        "last_restock_label",
        "quantity_whole_whole",
        "next_quantity_whole",
        "row_seq",
        "seq_no",
        (F.col("quantity_whole") - F.col("quantity_whole_rounded")).alias("quantity_whole_minus_quantity_whole_rounded"),
        (F.col("quantity_whole_rounded") - F.col("row_seq")).alias("quantity_whole_rounded_minus_row_seq"),
    )
)

base_inventory = (
    filtered
    .filter(F.col("quantity_whole_minus_quantity_whole_rounded").between(21, 70))
    .select(
        "id",
        "quantity_whole",
        "last_restock_label",
        "quantity_whole_whole",
        "next_quantity_whole",
        "row_seq",
        "seq_no",
        "quantity_whole_minus_quantity_whole_rounded",
        F.least("quantity_whole_whole", "quantity_whole_rounded_minus_row_seq").alias("least_quantity_whole_whole_quantity_whole_rounded_minus_row_seq"),
    )
)

result = (
    base_inventory
    .select(
        "id",
        "quantity_whole",
        "last_restock_label",
        "seq_no",
        "quantity_whole_minus_quantity_whole_rounded",
        F.avg("quantity_whole_minus_quantity_whole_rounded").over(w3).alias("quantity_whole_minus_quantity_whole_rounded_avg_2_prior"),
        F.cume_dist().over(w4).alias("seq_no_cume_dist"),
        F.percent_rank().over(w5).alias("seq_no_percent_rank"),
    )
)
