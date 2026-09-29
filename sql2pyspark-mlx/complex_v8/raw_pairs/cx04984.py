from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_start_d = Window.partitionBy("holder_id", "product").orderBy(F.col("start_date").asc_nulls_last())
w_holder_id_product_by_start_d_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status_by_start_date = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policies
    .filter((F.col("product").isin("HOME", "LIFE")) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

screened = (
    base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isNotNull()) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

sequenced = (
    screened
    .select(
        "*",
        F.round(F.percent_rank().over(w_holder_id_product_by_start_d), 3).alias("start_date_percent_rank"),
        F.lag("product_len", 2, 0).over(w_holder_id_product_by_start_d_2).alias("prev2_product_len"),
        F.ntile(3).over(w_holder_id_product_by_start_d_2).alias("start_date_tercile"),
        F.lead("start_date").over(w_status_by_start_date).alias("next_start_date"),
        F.last("start_date", ignorenulls=True).over(w_status_by_start_date_running).alias("last_known_start_date"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["start_date_tercile", "prev2_product_len"], ["start_date_tercile"], []],
        "start_date_tercile", "prev2_product_len",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
