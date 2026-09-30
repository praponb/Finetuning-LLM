from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_month_end", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id").orderBy("cost", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy("prev_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_count_event_seq", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy("last4_sum_prev_event_seq", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("product_id_min_event_seq", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("running_count_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("quantity") <= 180)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "product_id",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand").isin(["Acme", "Stark"]))
    .select(
        "e.id",
        "e.product_id",
        "e.event_seq",
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)

scored = (
    joined
    .select(
        "id",
        "event_seq",
        "launch_date",
        F.lag("cost").over(w3).alias("prev_cost"),
        F.lead("cost").over(w3).alias("next_cost"),
        F.lag("event_seq").over(w3).alias("prev_event_seq"),
        F.row_number().over(w3).alias("row_seq"),
        F.min("event_seq").over(w4).alias("product_id_min_event_seq"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "event_seq",
        "next_cost",
        "prev_event_seq",
        "row_seq",
        "product_id_min_event_seq",
        F.min("event_seq").over(w5).alias("running_min_event_seq"),
        F.avg("event_seq").over(w5).alias("running_avg_event_seq"),
        F.count("event_seq").over(w5).alias("running_count_event_seq"),
        F.last("row_seq", ignorenulls=True).over(w6).alias("last_known_row_seq"),
    )
)

scored_inventory = (
    metrics
    .select(
        "*",
        F.sum("prev_event_seq").over(w7).alias("last4_sum_prev_event_seq"),
        F.count("prev_event_seq").over(w7).alias("last4_count_prev_event_seq"),
        F.min("prev_event_seq").over(w7).alias("last4_min_prev_event_seq"),
        F.avg("prev_event_seq").over(w7).alias("last4_avg_prev_event_seq"),
    )
)

metrics_inventory = (
    scored_inventory
    .select(
        "*",
        F.avg("last4_min_prev_event_seq").over(w8).alias("last4_min_prev_event_seq_avg_2_prior"),
        F.avg("product_id_min_event_seq").over(w9).alias("product_id_min_event_seq_avg_3_prior"),
        F.first("last4_count_prev_event_seq", ignorenulls=True).over(w10).alias("first_known_last4_count_prev_event_seq"),
    )
)

grouped = (
    metrics_inventory
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("last4_min_prev_event_seq", [0.25, 0.5, 0.75]).alias("last4_min_prev_event_seq_quartiles"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)

result = grouped.select("row_seq_tier", "last4_min_prev_event_seq_quartiles")
