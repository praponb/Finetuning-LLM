from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")
w_ordered = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("last3_sum_rating").asc_nulls_first())
w5 = Window.orderBy(F.col("last3_avg_rating").desc())
w6 = Window.partitionBy("city")
w7 = Window.orderBy(F.col("running_rating").desc_nulls_last())

ranked = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .filter(F.col("city").isin(["Bangkok", "London", "Singapore"]))
    .select(
        "*",
        F.max("rating").over(w_ordered).alias("last3_max_rating"),
        F.avg("rating").over(w_ordered).alias("last3_avg_rating"),
        F.sum("rating").over(w_ordered).alias("last3_sum_rating"),
        F.min("rating").over(w_ordered).alias("last3_min_rating"),
        F.sum("rating").over(w_grp).alias("running_rating"),
    )
)

kept_keys = (
    metrics
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.length("supplier_name") < 10)
        .select("country", "city")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "country",
        "latest_rank",
        "last3_max_rating",
        "last3_sum_rating",
        "last3_min_rating",
        F.dense_rank().over(w4).alias("last3_sum_rating_dense_rank"),
        F.percent_rank().over(w5).alias("last3_avg_rating_percent_rank"),
        F.round((F.col("running_rating") / F.sum("running_rating").over(w6)), 4).alias("running_rating_share"),
        F.rank().over(w7).alias("running_rating_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") > 134), "left_anti")
)

result = (
    kept
    .cube("country")
    .agg(
        F.max("last3_sum_rating").alias("max_last3_sum_rating"),
        F.min("running_rating_share").alias("min_running_rating_share"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.grouping("country").alias("gid"),
    )
)
