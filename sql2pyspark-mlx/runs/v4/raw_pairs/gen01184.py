from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
    .orderBy(F.col("supplier_name_slug").desc())
)
