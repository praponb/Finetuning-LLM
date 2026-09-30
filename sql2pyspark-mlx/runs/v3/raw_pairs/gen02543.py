from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & ((F.col("opened_date") > "2023-09-01") | ~F.col("store_name").like("Mal%")))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.when(F.col("region") == "South", 1).otherwise(0).alias("is_south"),
        F.round((F.col("sqft") * 1.07), 2).alias("adjusted_sqft"),
    )
    .orderBy("store_name", "adjusted_sqft")
)
