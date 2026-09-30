from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity_whole").desc(), "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("last_restock")).orderBy("last_restock_minus_64d", "id")
w5 = Window.orderBy("quantity_whole_avg_2_prior", "id")
w6 = Window.orderBy("quantity_whole_avg_2_prior", "id").rowsBetween(-3, -1)
w_ordered = Window.orderBy(F.col("quantity_whole_avg_2_prior").asc_nulls_last())
w_grp = Window.orderBy(F.col("quantity_whole_bucket").desc())
w9 = Window.orderBy("next_quantity_whole_bucket", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-17").cast("date"))
    .select("id", "last_restock", F.floor("quantity").alias("quantity_whole"))
)

base = (
    valid
    .filter(F.col("last_restock") >= F.lit("2023-04-24").cast("date"))
    .withColumn("last_restock_minus_64d", F.date_sub("last_restock", 64))
)

scored = (
    base
    .select(
        "*",
        F.ntile(10).over(w).alias("quantity_whole_bucket"),
        F.avg("quantity_whole").over(w2).alias("quantity_whole_avg_2_prior"),
        F.first("quantity_whole", ignorenulls=True).over(w3).alias("first_known_quantity_whole"),
        F.lead("quantity_whole", 2).over(w4).alias("next_quantity_whole"),
    )
)

scored_inventory = (
    scored
    .withColumn("next_quantity_whole_bucket", F.lead("quantity_whole_bucket", 1, 0).over(w5))
    .withColumn("first_known_quantity_whole_avg_3_prior", F.avg("first_known_quantity_whole").over(w6))
)

base_inventory = (
    scored_inventory
    .filter(F.col("quantity_whole") > 34)
    .withColumn("next_quantity_whole_bucket_whole", F.floor("next_quantity_whole_bucket"))
)

scored_inventory_2 = (
    base_inventory
    .select(
        "*",
        F.percent_rank().over(w_ordered).alias("quantity_whole_avg_2_prior_percent_rank"),
        F.percent_rank().over(w_grp).alias("quantity_whole_bucket_percent_rank"),
        F.lag("next_quantity_whole_bucket", 2).over(w9).alias("prev_next_quantity_whole_bucket"),
    )
)

result = (
    scored_inventory_2
    .select(
        "id",
        "last_restock",
        "quantity_whole",
        "last_restock_minus_64d",
        "next_quantity_whole",
        "first_known_quantity_whole_avg_3_prior",
        "next_quantity_whole_bucket_whole",
        "quantity_whole_avg_2_prior_percent_rank",
        "quantity_whole_bucket_percent_rank",
    )
    .orderBy(F.col("first_known_quantity_whole_avg_3_prior").desc())
)
