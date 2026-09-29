from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")
w2 = Window.orderBy("sale_date_plus_months", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.month("sale_date_plus_months")).orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("cumulative_days_since_sale_date", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-11-01")
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
    )
)

filtered_store_sales = (
    filtered
    .filter(F.col("sale_date").between("2023-03-01", "2023-05-01"))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_plus_months",
        F.quarter("sale_date_plus_months").alias("sale_date_plus_months_quarter"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

scored = (
    filtered_store_sales
    .select(
        "id",
        "units",
        "revenue",
        "sale_date_plus_months_quarter",
        "days_since_sale_date",
        F.ntile(3).over(w).alias("revenue_bucket"),
        F.avg("sale_date_plus_months_quarter").over(w2).alias("sale_date_plus_months_quarter_avg_3_prior"),
        F.sum("days_since_sale_date").over(w3).alias("cumulative_days_since_sale_date"),
    )
)

scored_store_sales = (
    scored
    .withColumn("prev_cumulative_days_since_sale_date", F.lag("cumulative_days_since_sale_date").over(w_hist))
    .withColumn("next_cumulative_days_since_sale_date", F.lead("cumulative_days_since_sale_date").over(w_hist))
    .withColumn("prev_revenue_bucket", F.lag("revenue_bucket").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

kept_keys = (
    scored_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 6)
        .select("id")
    )
)

kept_rows = (
    scored_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("days_since_sale_date"), 2).alias("avg_days_since_sale_date"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first())
)
