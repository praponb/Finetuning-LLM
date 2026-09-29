from pyspark.sql import functions as F

result = (
    df_stores
    .cube(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "region",
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)
