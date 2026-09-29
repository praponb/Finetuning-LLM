from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("rating", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

with_windows = (
    df_suppliers
    .filter(F.col("country").isin(["US", "UK"]))
    .select(
        "id",
        "supplier_name",
        "city",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.avg("rating").over(w2).alias("rating_avg_2_prior"),
        F.avg("rating").over(w2).alias("rating_avg_2_prior_2"),
        F.ntile(10).over(w3).alias("rating_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "supplier_name",
        "city",
        "rating_avg_6_prior",
        "rating_avg_2_prior_2",
        F.when(F.col("city").rlike("^(Berlin|Tokyo)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
