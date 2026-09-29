from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNull() & F.col("brand").isin(["Initech", "Globex"]))
    .select(
        "id",
        "category",
        F.sort_array("tags").alias("sorted_tags"),
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
    )
    .orderBy("launch_date_label", "id")
    .limit(5)
)
