from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_running = Window.partitionBy("carrier", "status").orderBy("delivered_date_plus_months", "id")
w_hist = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.orderBy(F.col("delivered_date").desc(), "id")
w_recent = Window.partitionBy("status").orderBy("delivered_date", "id")
w6 = Window.partitionBy("status").orderBy(F.col("cost_share").desc())

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-03-13").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.dayofmonth("delivered_date").alias("delivered_date_day"),
    )
)

cleaned = (
    filtered
    .filter(F.col("status") != "LOST")
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").isin(["RETURNED", "IN_TRANSIT"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.add_months("delivered_date", 1).alias("delivered_date_plus_months"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("status") == "LOST")
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "status_group",
        "delivered_date_plus_months",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.row_number().over(w_running).alias("seq_no"),
        F.avg("cost").over(w_hist).alias("cost_avg_6_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share_2"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "cost",
        "status",
        "status_group",
        "seq_no",
        "row_seq",
        "cost_share_2",
        F.lag("seq_no").over(w_recent).alias("prev_seq_no"),
        F.lead("seq_no").over(w_recent).alias("next_seq_no"),
        F.lag("row_seq").over(w_recent).alias("prev_row_seq"),
        F.lead("row_seq").over(w_recent).alias("next_row_seq"),
        F.row_number().over(w_recent).alias("seq_no_2"),
        F.percent_rank().over(w6).alias("cost_share_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "status_group",
        "row_seq",
        "cost_share_2",
        "prev_seq_no",
        "next_seq_no",
        "prev_row_seq",
        "next_row_seq",
        "cost_share_percent_rank",
    )
)
