from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy("rating", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("rating_avg_6_prior").desc(), "id")

scored = (
    df_suppliers
    .select(
        "*",
        F.max("rating").over(w).alias("city_max_rating"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("rating").over(w3).alias("rating_avg_6_prior"),
        F.sum("rating").over(w_hist).alias("running_rating"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Acme"), "left_anti")
)

latest = (
    kept
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_max_rating",
        "rating_avg_6_prior",
        "running_rating",
        "row_num",
    )
    .orderBy("city")
)
