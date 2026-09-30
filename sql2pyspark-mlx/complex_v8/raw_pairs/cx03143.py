from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product = Window.partitionBy("status", "product")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc(), "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_status_by_premium_ties = Window.partitionBy("status").orderBy("premium")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

windowed = (
    scoped_policys
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_product), F.lit(0)), 4).alias("premium_share_of_status_product"),
        F.max("premium").over(w_status_product).alias("status_product_max_premium"),
        F.nth_value("premium", 2).over(w_status_holder_id_by_premium).alias("second_premium"),
        F.lead("premium").over(w_holder_id_product_by_premium).alias("next_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_holder_id), 2).alias("premium_vs_product_holder_id_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_holder_id), F.lit(0)), 4).alias("premium_share_of_product_holder_id"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.round(F.cume_dist().over(w_status_by_premium_ties), 3).alias("premium_cume_dist"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_product), 2).alias("premium_vs_status_product_avg"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.first("premium", ignorenulls=True).over(w_product_holder_id_by_premium).alias("first_known_premium"),
        F.row_number().over(w_holder_id_product_by_premium).alias("seq_no"),
    )
)

top_rows = windowed.filter(F.col("seq_no") == 1)

result = (
    top_rows
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "status_slug",
        "premium_share_of_status_product",
        "status_product_max_premium",
        "second_premium",
        "premium_vs_product_holder_id_avg",
        "premium_share_of_product_holder_id",
        "premium_cume_dist",
        "premium_vs_status_product_avg",
        "distinct_holder_id_in_status_product",
        "first_known_premium",
    )
    .orderBy(F.col("product").asc_nulls_last(), "id")
    .limit(5)
)
