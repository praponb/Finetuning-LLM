from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .orderBy("category")
)
