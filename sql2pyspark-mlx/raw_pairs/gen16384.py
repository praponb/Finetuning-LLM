from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        F.col("c.brand").alias("product_brand"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("rating") >= 4.2, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("product_brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy("num_rows")
)
