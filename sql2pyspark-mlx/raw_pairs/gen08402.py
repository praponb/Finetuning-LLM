from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

result = (
    long_format
    .groupBy(
        F.when(F.col("reading") >= 75, "LARGE").otherwise("SMALL").alias("reading_tier"),
    )
    .agg(
        F.size(F.collect_set("measure")).alias("num_measures"),
        F.countDistinct("measure").alias("distinct_measures"),
        F.sort_array(F.collect_set("measure")).alias("measure_list"),
    )
    .filter(F.col("num_measures") >= 1)
)
