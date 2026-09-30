from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("days_since_sale_date", "id")
w2 = Window.orderBy(F.col("next_units").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("month", "sale_date_plus_months")).orderBy("seq_no", "id")
w4 = Window.partitionBy(F.date_trunc("month", "sale_date_plus_months")).orderBy("seq_no", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("sale_date_plus_months", "id")
w6 = Window.orderBy("prev_units", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("prev_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("prev_units", "id")

base = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 6]))
    .select(
        "id",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
    )
)

metrics = (
    base
    .filter(F.col("days_since_sale_date") != 24)
    .select(
        "*",
        F.lead("days_since_sale_date").over(w_recent).alias("next_days_since_sale_date"),
        F.lag("days_since_sale_date").over(w_recent).alias("prev_days_since_sale_date"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.lag("units").over(w_recent).alias("prev_units"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lead("units", 1).over(w_recent).alias("next_units_2"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("next_units", 1, 0).over(w3).alias("next_next_units"),
        F.avg("next_units").over(w4).alias("next_units_avg_6_prior"),
    )
)

enriched_store_sales = (
    enriched
    .select(
        "id",
        "units",
        "days_since_sale_date",
        "prev_days_since_sale_date",
        "seq_no",
        "next_units_2",
        "event_seq",
        "next_next_units",
        "next_units_avg_6_prior",
        F.lag("prev_units", 1, 0).over(w5).alias("prev_prev_units"),
        F.avg("prev_days_since_sale_date").over(w6).alias("prev_days_since_sale_date_avg_6"),
        F.first("next_days_since_sale_date", ignorenulls=True).over(w7).alias("first_known_next_days_since_sale_date"),
        F.ntile(3).over(w8).alias("prev_units_bucket"),
    )
)

result = (
    enriched_store_sales
    .select(
        "id",
        "days_since_sale_date",
        "seq_no",
        "next_next_units",
        "prev_days_since_sale_date_avg_6",
        "prev_units_bucket",
    )
    .orderBy("days_since_sale_date")
)
