from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_running = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("status_or_unknown", "holder_id").orderBy("end_date", "id")

prepared_policys = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

sequenced = (
    prepared_policys
    .select(
        "*",
        F.round(F.col("end_date_dayofweek") - F.avg("end_date_dayofweek").over(w_product), 2).alias("end_date_dayofweek_vs_product_avg"),
        F.nth_value("premium_filled", 2).over(w_all_by_end_date_full).alias("second_premium_filled"),
        F.first("end_date", ignorenulls=True).over(w_all_by_end_date_running).alias("first_known_end_date"),
        F.round(F.avg("premium_filled").over(w_product), 2).alias("product_avg_premium_filled"),
    )
)

unique_rows = (
    sequenced
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "product_avg_premium_filled",
        "end_date_dayofweek_vs_product_avg",
        "premium_filled",
        "premium",
    )
    .unpivot(
        ids=["id"],
        values=["product_avg_premium_filled", "end_date_dayofweek_vs_product_avg", "premium_filled", "premium"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
