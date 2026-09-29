from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_label_by_category = Window.partitionBy("is_active_label").orderBy("category_len", "id")
w_category_len = Window.partitionBy("category_len")

scoped = (
    df_skus
    .filter(~F.array_contains(F.col("labels"), "bulky"))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "is_active",
        F.greatest(F.col("weight_kg"), F.col("unit_cost")).alias("greatest_weight_kg_unit_cost"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
)

derived = (
    scoped
    .filter(F.col("unit_cost") > 68.0)
    .select(
        "id",
        "sku_code",
        "is_active",
        "greatest_weight_kg_unit_cost",
        "is_active_label",
        "category_len",
        F.concat_ws(": ", F.col("sku_code"), F.col("is_active_label")).alias("sku_code_is_active_label_label"),
        F.length(F.trim(F.col("is_active_label"))).alias("is_active_label_len"),
        F.when(F.col("category_len") >= 10, "HIGH").when(F.col("category_len") >= 7, "MEDIUM").otherwise("LOW").alias("category_len_tier"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "is_active",
        "greatest_weight_kg_unit_cost",
        "is_active_label",
        "category_len",
        "sku_code_is_active_label_label",
        "is_active_label_len",
        (F.col("is_active_label_len") - F.lag("is_active_label_len").over(w_is_active_label_by_category)).alias("is_active_label_len_change"),
        F.round(F.col("greatest_weight_kg_unit_cost") - F.avg("greatest_weight_kg_unit_cost").over(w_category_len), 2).alias("greatest_weight_kg_unit_cost_vs_category_len_avg"),
        F.round(F.avg("greatest_weight_kg_unit_cost").over(w_category_len), 2).alias("category_len_avg_greatest_weight_kg_unit_cost"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "category_len",
        "is_active_label_len",
        "is_active_label_len_change",
    )
    .unpivot(
        ids=["id"],
        values=["category_len", "is_active_label_len", "is_active_label_len_change"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
