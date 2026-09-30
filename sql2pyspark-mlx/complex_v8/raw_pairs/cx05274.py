from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_end_dat = Window.partitionBy("holder_id", "product").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product_by_end_dat_2 = Window.partitionBy("holder_id", "product").orderBy("end_date", "id")
w_holder_id_product_by_end_dat_3 = Window.partitionBy("holder_id", "product").orderBy("end_date")

filtered = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

joined_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name")
)

sequenced = (
    joined_holder
    .select(
        "*",
        F.first("end_date", ignorenulls=True).over(w_holder_id_product_by_end_dat).alias("first_known_end_date"),
        F.ntile(3).over(w_holder_id_product_by_end_dat_2).alias("end_date_tercile"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_product_by_end_dat_2)).alias("days_since_prev_end_date"),
        F.rank().over(w_holder_id_product_by_end_dat_3).alias("end_date_rank"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "holder_name",
        "first_known_end_date",
        "end_date_tercile",
        "end_date_rank",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

result = (
    tagged
    .rollup("end_date_rank", "end_date_tercile", "status")
    .agg(
        F.grouping("end_date_rank").alias("is_total_end_date_rank"),
        F.sum(F.when(F.col("holder_name").isNotNull(), F.col("end_date_rank")).otherwise(0)).alias("end_date_rank_holder_name_is_not_null"),
        F.sum("end_date_tercile").alias("total_end_date_tercile"),
        F.round(100 * F.count(F.when(F.col("status").isin("EXPIRED", "CANCELLED"), 1)) / F.count("*"), 1).alias("pct_status_in_expired_cancelled"),
    )
    .orderBy(
        F.col("end_date_rank").asc_nulls_last(),
        F.col("end_date_tercile").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
