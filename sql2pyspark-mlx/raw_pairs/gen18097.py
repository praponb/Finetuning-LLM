from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "tags",
        F.sum("cost").over(w_grp).alias("last4_sum_cost"),
        F.count("cost").over(w_grp).alias("last4_count_cost"),
        F.max("cost").over(w_grp).alias("last4_max_cost"),
        F.avg("cost").over(w_grp).alias("last4_avg_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "last4_max_cost",
        "last4_avg_cost",
        F.when(F.col("product_name").rlike("^(USB-C Hub|Desk Lamp)$"), "CORE").otherwise("EXTENDED").alias("product_name_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
