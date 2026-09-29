from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy("city").orderBy("running_avg_rating", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("running_avg_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("running_count_rating").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").isNotNull()), "left_anti")
)

scored = (
    kept
    .filter(F.length("supplier_name") >= 14)
    .select(
        "*",
        F.count("rating").over(w_ordered).alias("running_count_rating"),
        F.avg("rating").over(w_ordered).alias("running_avg_rating"),
        F.sum("rating").over(w_ordered).alias("running_sum_rating"),
        F.ntile(4).over(w2).alias("rating_bucket"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.lead("running_sum_rating", 2, 0).over(w3).alias("next_running_sum_rating"),
        F.avg("rating").over(w4).alias("rating_avg_3"),
        F.sum("rating").over(w5).alias("running_rating"),
        F.first("running_avg_rating", ignorenulls=True).over(w6).alias("first_known_running_avg_rating"),
        F.cume_dist().over(w7).alias("running_count_rating_cume_dist"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.running_count_rating",
        "s.running_sum_rating",
        "s.first_known_running_avg_rating",
        F.col("c.brand"),
    )
)

result = (
    with_latest
    .groupBy("brand")
    .agg(
        F.avg(F.when(F.col("brand") == "Initech", F.col("first_known_running_avg_rating"))).alias("initech_avg_first_known_running_avg_rating"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("first_known_running_avg_rating")).otherwise(0)).alias("initech_first_known_running_avg_rating"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.round(F.avg("running_sum_rating"), 2).alias("avg_running_sum_rating"),
    )
    .filter(F.col("initech_first_known_running_avg_rating") > 0)
)
